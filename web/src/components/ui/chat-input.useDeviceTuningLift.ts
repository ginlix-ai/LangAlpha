import { useEffect, useRef } from 'react';
import type { useModelProfileWriter } from '@/hooks/useModelProfile';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { modelProfile } from '@/lib/modelPreferences';
import type { ModelProfile } from '@/lib/modelTuning';

/**
 * A model's reasoning effort and fast mode used to be device-local picks,
 * which meant server-started turns never saw them. Lift what this device still
 * holds into the account profile, then drop the keys. Per field, not per record: a profile that already carries one
 * of the two must not strand the other. And the keys are this value's only
 * copy until the write lands, so they go only once it has. The pill's model,
 * not the selection: with nothing saved the default is what runs, and its
 * keys are the ones this device has been using.
 */
export function useDeviceTuningLift({
  pillModel,
  preferences,
  modelMetadata,
  reasoningEfforts,
  writeProfile,
}: {
  pillModel: string | null;
  preferences: unknown;
  modelMetadata: Record<string, ModelMetadataEntry>;
  reasoningEfforts: string[];
  writeProfile: ReturnType<typeof useModelProfileWriter>['writeProfile'];
}) {
  const liftedModelRef = useRef<string | null>(null);
  useEffect(() => {
    // The model's own row has to be in hand first: `reasoningEfforts` reads
    // empty both while the catalog loads and for a model that declares no
    // ladder, and the clamp below wants opposite answers for the two.
    const meta = pillModel ? modelMetadata[pillModel] : undefined;
    if (!pillModel || !preferences || !meta || liftedModelRef.current === pillModel) return;
    liftedModelRef.current = pillModel;

    const profile = modelProfile(preferences, pillModel);
    const storedEffort = localStorage.getItem(`reasoning_effort:${pillModel}`);
    const storedFast = localStorage.getItem(`fast_mode:${pillModel}`) === 'true';
    const lift: ModelProfile = {};
    // These keys predate per-model ladders, when one fixed list of levels was
    // offered for every model, so a device may hold a level this model never
    // declared. The account rejects it and a run would drop it anyway, so it
    // is discarded with the keys rather than lifted.
    if (storedEffort && reasoningEfforts.includes(storedEffort) && profile.reasoning_effort === undefined) {
      lift.reasoning_effort = storedEffort;
    }
    if (storedFast && profile.fast_mode === undefined) lift.fast_mode = true;

    const dropDeviceKeys = () => {
      localStorage.removeItem(`reasoning_effort:${pillModel}`);
      localStorage.removeItem(`fast_mode:${pillModel}`);
    };
    if (Object.keys(lift).length > 0) {
      // No release of the guard on failure: the write moves `preferences`
      // twice, so re-entry is immediate and a rejection would resubmit itself
      // without bound. A failed lift keeps its device keys and is retried on
      // the next mount or model switch.
      writeProfile(pillModel, lift, { onSuccess: dropDeviceKeys });
    } else {
      dropDeviceKeys();
    }
  }, [pillModel, preferences, writeProfile, modelMetadata, reasoningEfforts]);
}
