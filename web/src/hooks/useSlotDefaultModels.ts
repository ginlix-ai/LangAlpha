import { useAllModels } from './useAllModels';
import { usePreferences } from './usePreferences';
import { slotDefaultModel, modelPrefs, type ModelSlot } from '@/lib/modelPreferences';
import { isPlatformMode } from '@/config/hostMode';

/**
 * The account's default model for each slot, as the composer, the
 * thread model controls and a default change all read it.
 *
 * `null` until the preference is read: before that the server may run a
 * stored model this client cannot name, and claiming the deployment default
 * in its place would send that model, and its tuning, by mistake. A read that
 * found no preferences row (a reset deletes it) is known, and runs the
 * deployment's defaults.
 */
export function useSlotDefaultModels(): Record<ModelSlot, string | null> {
  const { preferences, isLoaded } = usePreferences();
  const { systemDefaults } = useAllModels();
  if (!isLoaded) return { default: null, background: null };
  const prefs = modelPrefs(preferences);
  return {
    default: slotDefaultModel(prefs, systemDefaults, 'default'),
    background: slotDefaultModel(prefs, systemDefaults, 'background'),
  };
}

/**
 * The model every composer and thread starts on: the account's Default slot,
 * or the deployment's while the user's plan does not cover the saved one. A
 * downgrade leaves the paid model saved, and the server refuses a turn that
 * would run it on the platform's key.
 *
 * `null` until the catalog and the plan's answer load, and on a platform where
 * either did not come, since without both an off-plan model reads as on plan;
 * a send naming none runs what the server resolves.
 */
export function useDefaultModel(): string | null {
  const saved = useSlotDefaultModels().default;
  const { offPlanModels, systemDefaults, platform, rawApiResponse, isLoading } = useAllModels();
  if (saved === null || isLoading || (isPlatformMode && !(platform && rawApiResponse))) return null;
  return offPlanModels.has(saved) ? systemDefaults?.default_model ?? null : saved;
}
