/**
 * Merge a new batch of custom_models entries into the existing preference
 * list, preserving any entries the user added under the same slug elsewhere
 * (e.g. via ModelPickStep's "Add model" form).
 *
 * Invariant: after the merge, the slug's entries are the union of
 *   (preserved existing-for-this-slug entries whose name is NOT in the new batch)
 *   ++ (all new batch entries)
 *
 * Entries for OTHER slugs are passed through untouched.
 *
 * Used by ConnectStep.handleCustomSave so re-entering "Add new custom provider"
 * with an existing slug doesn't wipe the user's pre-existing entries.
 */
export interface CustomModelEntry {
  name: string;
  model_id?: string;
  provider: string;
  input_modalities?: string[];
  [key: string]: unknown;
}

export interface MergeInput {
  /** Current ``custom_models`` array from preferences (all slugs). */
  existing: CustomModelEntry[];
  /** Target provider slug being (re)written. */
  slug: string;
  /** The wizard's new entries for this slug. Wins on name collision with existing-for-slug. */
  newForSlug: CustomModelEntry[];
}

export function mergeCustomModelsForSlug({
  existing,
  slug,
  newForSlug,
}: MergeInput): CustomModelEntry[] {
  const newNames = new Set(newForSlug.map((m) => m.name));
  const otherSlugs = existing.filter((m) => m.provider !== slug);
  const preservedForSlug = existing.filter(
    (m) => m.provider === slug && !newNames.has(m.name),
  );
  return [...otherSlugs, ...preservedForSlug, ...newForSlug];
}
