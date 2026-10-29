import { useCallback } from 'react';
import { splitPreferenceWrite, type PreferencesLike } from '@/lib/modelPreferences';
import {
  favoriteModels, ownModelNames, HIDDEN_KEY, STARRED_KEY, STARRED_ONLY_KEY,
} from '@/lib/favoriteModels';
import { isModelAvailable } from '@/components/ui/chat-input.models';
import { useAllModels } from './useAllModels';
import { useDefaultModel } from './useSlotDefaultModels';
import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

/**
 * The user's favorite models, the ones they can reach now, and the edits to
 * the list. The seed rule and the order live in `lib/favoriteModels`.
 */
export function useFavoriteModels() {
  const { preferences, isLoaded } = usePreferences();
  const {
    models: visibleModels, payers, keylessModels, validModelNames, isLoading: modelsLoading,
  } = useAllModels();
  const { mutate } = useUpdatePreferences();
  // The default as the composer resolves it, the deployment's when nothing is
  // saved: a pick on a thread saves no preference, so this can be the only
  // way back to a default nobody chose.
  const defaultModel = useDefaultModel();

  // starred_models stayed in other_preference when model routing moved to its
  // own column.
  const other = (preferences as PreferencesLike | null)?.other_preference ?? {};
  const saved = favoriteModels({
    other,
    preferredModel: defaultModel,
    ownModels: ownModelNames(visibleModels, payers, keylessModels),
  });
  // An unreachable favorite stays saved, so it is back once its key or plan is.
  const models = saved.filter((m) => isModelAvailable(m, validModelNames));

  // Each edit replaces the list whole and ends the seed, so one built on
  // preferences not yet read would erase the saved ones, and one built before
  // the models and their payers load would drop the user's own models from
  // the seed for good. It waits for both.
  const write = useCallback((next: string[]) => {
    if (!isLoaded || modelsLoading) return;
    mutate(splitPreferenceWrite({
      [STARRED_KEY]: next,
      [HIDDEN_KEY]: null,
      [STARRED_ONLY_KEY]: true,
    }));
  }, [isLoaded, modelsLoading, mutate]);

  const add = useCallback((model: string) => {
    if (!saved.includes(model)) write([...saved, model]);
  }, [saved, write]);
  const remove = useCallback((model: string) => write(saved.filter((m) => m !== model)), [saved, write]);

  return { models, add, remove };
}
