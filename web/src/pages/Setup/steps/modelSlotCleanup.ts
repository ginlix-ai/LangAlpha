/**
 * Slot cleanup after the user deselects a model in ModelPickStep.
 *
 * When a user unchecks a model that was previously filling a dedicated slot
 * (``preferred_model``, ``preferred_flash_model``, ``compaction_model``,
 * ``fetch_model``, or any entry of ``fallback_models``), the slot keeps
 * pointing at the now-hidden model and the chat dropdown re-seeds from it.
 * That's the regression — stale picks appear selected even after the user
 * removed them from their configured list.
 *
 * This helper computes the minimal patch: any slot whose current value was a
 * model in THIS provider AND is no longer in the merged configured set is
 * nulled out. Slots pointing at models from other providers stay untouched.
 */

export interface OtherPrefLike {
  preferred_model?: string | null;
  preferred_flash_model?: string | null;
  compaction_model?: string | null;
  fetch_model?: string | null;
  fallback_models?: string[];
  [key: string]: unknown;
}

export interface SlotCleanupPatch {
  /** Null-valued overrides for scalar slots whose current pick is orphaned. */
  nulls: Partial<Record<'preferred_model' | 'preferred_flash_model' | 'compaction_model' | 'fetch_model', null>>;
  /** New fallback list with orphaned entries removed — only set when changed. */
  fallback_models?: string[];
}

export interface SlotCleanupInput {
  /** Slot values to evaluate (typically ``otherPref`` from preferences). */
  otherPref: OtherPrefLike;
  /** All models shown in the current ModelPickStep (built-in + any custom for this provider). */
  allModels: string[];
  /** The union of this-step selections and the user's picks for other providers. */
  mergedConfigured: string[];
}

export function computeSlotCleanup({
  otherPref,
  allModels,
  mergedConfigured,
}: SlotCleanupInput): SlotCleanupPatch {
  const inThisProviderModels = new Set(allModels);
  const selectedSet = new Set(mergedConfigured);

  const isOrphaned = (m: string | null | undefined): boolean =>
    typeof m === 'string' && inThisProviderModels.has(m) && !selectedSet.has(m);

  const nulls: SlotCleanupPatch['nulls'] = {};
  if (isOrphaned(otherPref.preferred_model)) nulls.preferred_model = null;
  if (isOrphaned(otherPref.preferred_flash_model)) nulls.preferred_flash_model = null;
  if (isOrphaned(otherPref.compaction_model)) nulls.compaction_model = null;
  if (isOrphaned(otherPref.fetch_model)) nulls.fetch_model = null;

  const existingFallback = otherPref.fallback_models ?? [];
  const cleanedFallback = existingFallback.filter((m) => !isOrphaned(m));
  const fallbackChanged = cleanedFallback.length !== existingFallback.length;

  return fallbackChanged
    ? { nulls, fallback_models: cleanedFallback }
    : { nulls };
}
