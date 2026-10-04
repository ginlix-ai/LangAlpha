/**
 * Which models the chat input offers for quick switching.
 *
 * The policy behind `useQuickAccessModels`, kept free of React so the order
 * and the opt-out rule are stated and tested once.
 */
import { isModelAvailable } from '@/components/ui/chat-input.models';
import type { ModelAccess } from '@/types/platform';

/** A stored list of model names. Preferences are free-form JSON, so anything
 *  else reads as no list rather than reaching a label lookup. */
export function modelList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((m): m is string => typeof m === 'string' && m.length > 0)
    : [];
}

/**
 * The listed models the user reaches through their own key or a connected
 * account, in catalog order.
 *
 * Read from the access map the badges use. Without a platform there is no
 * map, and every listed model is already one of these: the list then carries
 * only the providers the user configured and the models they added.
 */
export function ownModelNames(
  models: Record<string, { models?: string[] }>,
  accessMap: Record<string, ModelAccess> | undefined,
): string[] {
  const names = Object.values(models).flatMap((group) => group.models ?? []);
  const own = accessMap
    ? names.filter((m) => accessMap[m] === 'byok' || accessMap[m] === 'oauth')
    : names;
  return [...new Set(own)];
}

export interface QuickAccessParams {
  preferredModel: string | null | undefined;
  preferredFlashModel: string | null | undefined;
  starredModels: string[];
  /** Models from the user's own keys and connected accounts. */
  ownModels: string[];
  /** Models the user removed from the list, which no rule above adds back. */
  hiddenModels: string[];
  validModelNames: Set<string>;
  /** Models already shown in the menu's primary section; excluded to avoid duplicate rows. */
  excludeModels?: string[];
}

/**
 * Quick-access models for the chat model menu: the current primary + flash
 * defaults, the user's manual stars, then the models of their own keys and
 * connected accounts, less the ones they removed, gated by availability
 * (drops removed/revoked models). Derived per-render so switching a default
 * or connecting an account never leaves the list behind.
 */
export function deriveQuickAccessModels({
  preferredModel,
  preferredFlashModel,
  starredModels,
  ownModels,
  hiddenModels,
  validModelNames,
  excludeModels = [],
}: QuickAccessParams): string[] {
  const exclude = new Set([...excludeModels, ...hiddenModels]);
  const union = [...new Set(
    // typeof check (not just `!!m`) so a malformed pref with non-string entries
    // can't reach getModelDisplayName's `key.startsWith` and crash the composer.
    [preferredModel, preferredFlashModel, ...starredModels, ...ownModels].filter(
      (m): m is string => typeof m === 'string' && m.length > 0,
    ),
  )];
  return union.filter((m) => {
    // Already rendered in the primary section (selected + thread models), or
    // taken off the list by the user.
    if (exclude.has(m)) return false;
    return isModelAvailable(m, validModelNames);
  });
}
