/**
 * Which models the user keeps as favorites, the model picker's first tab.
 *
 * The policy behind `useFavoriteModels`, kept free of React so the seed and
 * the order are stated and tested once.
 */
import type { Payer } from '@/lib/modelPayer';

export const STARRED_KEY = 'starred_models';
/** Models taken off the list before favorites were starred-only. */
export const HIDDEN_KEY = 'hidden_quick_access_models';
/** Set by the first edit: from then on the stars alone are the list. */
export const STARRED_ONLY_KEY = 'favorites_starred_only';

/** A stored list of model names. Preferences are free-form JSON, so anything
 *  else reads as no list rather than reaching a label lookup. */
export function modelList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((m): m is string => typeof m === 'string' && m.length > 0)
    : [];
}

/**
 * The listed models the user reaches through their own key or a connected
 * account, in catalog order: every listed model credits do not pay for.
 *
 * Read from the payers the badges show. On a self-hosted deployment that is
 * every listed model, the deployment's keys and local servers included: the
 * list carries only the providers the user configured and the models they
 * added. A model they added that no key of theirs serves now is left out:
 * listed, but a turn on it is refused.
 */
export function ownModelNames(
  models: Record<string, { models?: string[] }>,
  payers: Record<string, Payer>,
  keyless: ReadonlySet<string> = new Set(),
): string[] {
  const names = Object.values(models).flatMap((group) => group.models ?? []);
  return [...new Set(names.filter((m) => payers[m] !== 'credits' && !keyless.has(m)))];
}

/**
 * The favorites in their saved order, reachable or not.
 *
 * Until the user first edits the list it is the quick list the composer used
 * to derive: the default model, the stars, then the models of their own keys
 * and accounts, less the ones they removed. The first edit saves that list as
 * the stars, so a user keeps what they saw, and from then on a newly connected
 * account adds nothing on its own.
 */
export function favoriteModels({ other, preferredModel, ownModels }: {
  other: Record<string, unknown>;
  preferredModel: string | null | undefined;
  ownModels: string[];
}): string[] {
  const starred = modelList(other[STARRED_KEY]);
  if (other[STARRED_ONLY_KEY] === true) return [...new Set(starred)];
  const hidden = new Set(modelList(other[HIDDEN_KEY]));
  return [...new Set(modelList([preferredModel, ...starred, ...ownModels]))]
    .filter((m) => !hidden.has(m));
}
