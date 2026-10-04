import { useAllModels } from './useAllModels';
import { usePreferences } from './usePreferences';
import { modeDefaultModel, modelPrefs, type ComposerMode } from '@/lib/modelPreferences';

/**
 * The account's default model for each composer mode, as the composer, the
 * thread model controls and a default change all read it.
 *
 * `null` until the preference is read: before that the server may run a
 * stored model this client cannot name, and claiming the deployment default
 * in its place would send that model, and its tuning, by mistake. A read that
 * found no preferences row (a reset deletes it) is known, and runs the
 * deployment's defaults.
 */
export function useModeDefaultModels(): Record<ComposerMode, string | null> {
  const { preferences, isLoaded } = usePreferences();
  const { systemDefaults } = useAllModels();
  if (!isLoaded) return { ptc: null, fast: null };
  const prefs = modelPrefs(preferences);
  return {
    ptc: modeDefaultModel(prefs, systemDefaults, 'ptc'),
    fast: modeDefaultModel(prefs, systemDefaults, 'fast'),
  };
}

/** The account's default model for `mode`; anything but fast reads as PTC. */
export function useModeDefaultModel(mode: ComposerMode | undefined): string | null {
  return useModeDefaultModels()[mode === 'fast' ? 'fast' : 'ptc'];
}
