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

/**
 * The model a composer opens on: the thread's own while the catalog still
 * carries it, else the mode's default.
 *
 * `retired` names a thread model the catalog dropped, so the host can say why
 * the composer moved off it instead of switching silently. A send then names
 * the default, which is also what the server runs for a dead model. Judged on
 * the catalog, not on reach: a model behind a lapsed connection is still the
 * thread's, and its send gets the server's own reason it cannot run. An empty
 * catalog is one not loaded yet, under which nothing is called retired.
 */
export function resolveComposerModel(
  threadModel: string | null | undefined,
  modeDefault: string | null,
  catalogModelNames: Set<string>,
): { seed: string | null; retired: string | null } {
  if (threadModel && isModelAvailable(threadModel, catalogModelNames)) {
    return { seed: threadModel, retired: null };
  }
  return { seed: modeDefault, retired: threadModel || null };
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
