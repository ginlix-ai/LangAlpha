/**
 * Pure model-selection helpers for the chat input's model menu.
 *
 * Kept dependency-free (no React, no API/auth imports) so the logic is unit
 * testable without pulling in the full `chat-input` component graph.
 */

/**
 * Whether the user can still reach *model*.
 *
 * Stays open while the model list is empty so a slow or failed fetch degrades to
 * showing everything rather than blanking the menu.
 */
export function isModelAvailable(model: string, validModelNames: Set<string>): boolean {
  return validModelNames.size === 0 || validModelNames.has(model);
}

export interface PrimaryModelsParams {
  selectedModel: string | null;
  /** Models this thread has already used, from replayed turn metadata. */
  threadModels: string[];
  validModelNames: Set<string>;
}

/**
 * Models listed in the menu's primary section: the ones this thread already used,
 * then the current selection.
 *
 * Thread history is gated on availability because a model can be revoked long
 * after a turn used it, and an unreachable row only fails once the user sends.
 * The selection itself is never gated — the trigger displays it, so dropping it
 * would leave the menu unable to show what is currently selected.
 */
export function derivePrimaryModels({
  selectedModel,
  threadModels,
  validModelNames,
}: PrimaryModelsParams): string[] {
  const reachable = threadModels.filter(
    (m) => typeof m === 'string' && m.length > 0 && isModelAvailable(m, validModelNames),
  );
  return [...new Set([...reachable, selectedModel].filter((m): m is string => !!m))];
}

export interface QuickAccessParams {
  preferredModel: string | null;
  preferredFlashModel: string | null;
  starredModels: string[];
  validModelNames: Set<string>;
  /** Models already shown in the menu's primary section; excluded to avoid duplicate rows. */
  excludeModels?: string[];
}

/**
 * Quick-access models for the chat model menu — the current primary + flash
 * defaults unioned with the user's manual stars, gated by availability (drops
 * removed/revoked models). Derived per-render so switching a default never
 * leaves a stale pin behind.
 */
export function deriveQuickAccessModels({
  preferredModel,
  preferredFlashModel,
  starredModels,
  validModelNames,
  excludeModels = [],
}: QuickAccessParams): string[] {
  const exclude = new Set(excludeModels);
  const union = [...new Set(
    // typeof check (not just `!!m`) so a malformed pref with non-string entries
    // can't reach getModelDisplayName's `key.startsWith` and crash the composer.
    [preferredModel, preferredFlashModel, ...starredModels].filter(
      (m): m is string => typeof m === 'string' && m.length > 0,
    ),
  )];
  return union.filter((m) => {
    // Already rendered in the primary section (selected + thread models).
    if (exclude.has(m)) return false;
    return isModelAvailable(m, validModelNames);
  });
}
