import { useCallback, useMemo } from 'react';
import { useUpdatePreferences } from './useUpdatePreferences';
import { usePreferences } from './usePreferences';
import { useAllModels } from './useAllModels';
import { modelPrefs, modelProfile } from '@/lib/modelPreferences';
import { resolveTuning } from '@/lib/modelTuning';
import type { ModelProfile } from '@/lib/modelTuning';

/**
 * Writers for the two layers of model configuration.
 *
 * Both send a patch, never the whole bag: the server merges `model_preference`
 * three levels deep, so writing one model's effort leaves its siblings and that
 * model's other settings alone. `null` at either level deletes.
 */
export function useModelProfileWriter() {
  const { mutate } = useUpdatePreferences();

  /** Override one model. Pass `{ field: null }` to drop back to the account value.
   *
   * The callbacks exist for callers that hand over the only copy of a value:
   * they cannot discard their own until the server has it, and cannot leave a
   * one-shot guard standing over a write that never landed.
   */
  const writeProfile = useCallback(
    (
      model: string,
      patch: ModelProfile,
      options?: { onSuccess?: () => void; onError?: () => void },
    ) => {
      mutate({ model_preference: { profiles: { [model]: patch } } }, options);
    },
    [mutate],
  );

  /** Drop a model's overrides entirely. */
  const clearProfile = useCallback(
    (model: string) => {
      mutate({ model_preference: { profiles: { [model]: null } } });
    },
    [mutate],
  );

  /** The account-wide value a model inherits when it has no override. */
  const writeAccountDefault = useCallback(
    (patch: ModelProfile) => {
      mutate({ model_preference: patch });
    },
    [mutate],
  );

  return { writeProfile, clearProfile, writeAccountDefault };
}

/**
 * One model's tuning, read the way the server resolves it.
 *
 * The composer and the settings matrix both have to answer "what does this
 * control show, and what happens if I leave it alone?", and they used to
 * compose the three layers separately, with two different tests for whether
 * the account had a value at all.
 */
export function useEffectiveTuning(model: string | null | undefined) {
  const { preferences } = usePreferences();
  const { metadata, systemDefaults } = useAllModels();
  const pinned = systemDefaults?.prompt_guidance ?? null;
  return useMemo(
    () =>
      resolveTuning(
        modelPrefs(preferences),
        modelProfile(preferences, model),
        model ? metadata[model] : null,
        pinned,
      ),
    [preferences, metadata, model, pinned],
  );
}
