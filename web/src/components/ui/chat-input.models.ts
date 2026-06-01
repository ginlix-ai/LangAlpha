/**
 * Pure model-selection helpers for the chat input's model menu.
 *
 * Kept dependency-free (no React, no API/auth imports) so the logic is unit
 * testable without pulling in the full `chat-input` component graph.
 */

export type ModelMetadata = Record<string, { sdk?: string; provider?: string }>;

/**
 * Check if two models are compatible for mid-session switching.
 * - Different SDKs → incompatible
 * - openai/codex SDK → must be same provider (sub-provider)
 * - Other SDKs (anthropic, gemini, etc.) → compatible if same SDK
 */
export function areModelsCompatible(modelA: string | null, modelB: string | null, metadata: ModelMetadata): boolean {
  if (!modelA || !modelB) return true;
  const a = metadata[modelA], b = metadata[modelB];
  if (!a || !b) return true; // unknown models → allow
  if (a.sdk !== b.sdk) return false;
  if (a.sdk === 'openai' || a.sdk === 'codex') {
    return a.provider === b.provider;
  }
  return true;
}

export interface QuickAccessParams {
  preferredModel: string | null;
  preferredFlashModel: string | null;
  starredModels: string[];
  validModelNames: Set<string>;
  initialModel: string | null;
  selectedModel: string | null;
  modelMetadata: ModelMetadata;
  /** Models already shown in the menu's primary section; excluded to avoid duplicate rows. */
  excludeModels?: string[];
}

/**
 * Quick-access models for the chat model menu — the current primary + flash
 * defaults unioned with the user's manual stars, gated by availability (drops
 * removed/revoked models) and, mid-thread, by SDK compatibility. Derived
 * per-render so switching a default never leaves a stale pin behind.
 */
export function deriveQuickAccessModels({
  preferredModel,
  preferredFlashModel,
  starredModels,
  validModelNames,
  initialModel,
  selectedModel,
  modelMetadata,
  excludeModels = [],
}: QuickAccessParams): string[] {
  const exclude = new Set(excludeModels);
  const union = [...new Set(
    [preferredModel, preferredFlashModel, ...starredModels].filter((m): m is string => !!m),
  )];
  return union.filter((m) => {
    // Already rendered in the primary section (selected + thread models).
    if (exclude.has(m)) return false;
    // Skip the availability gate while the model list is still loading (empty
    // set) so a slow/failed fetch can't blank the menu.
    if (validModelNames.size > 0 && !validModelNames.has(m)) return false;
    // Mid-thread, only offer models compatible with the thread's model.
    return !initialModel || areModelsCompatible(selectedModel, m, modelMetadata);
  });
}
