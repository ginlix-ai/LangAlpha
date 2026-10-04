import { useCallback } from 'react';
import { isPlatformMode } from '@/config/hostMode';
import { splitPreferenceWrite, type PreferencesLike } from '@/lib/modelPreferences';
import { deriveQuickAccessModels, modelList, ownModelNames } from '@/lib/quickAccessModels';
import { useAllModels } from './useAllModels';
import { useModeDefaultModels } from './useModeDefaultModel';
import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

const STARRED_KEY = 'starred_models';
const HIDDEN_KEY = 'hidden_quick_access_models';

/**
 * The models the chat input offers for quick switching, and the edits to that
 * list.
 *
 * The list starts full rather than empty: the default models and every model
 * of the user's own keys and connected accounts are in it unstarred, beside
 * the ones starred by hand. Removing a model records it in
 * `hidden_quick_access_models`, which outranks every rule that lists it, so an
 * opt-out holds when a default changes or an account reconnects. Adding one
 * stars it and lifts the opt-out, so it stays once the rule that listed it
 * stops applying.
 *
 * `exclude` drops models the caller already shows elsewhere.
 */
export function useQuickAccessModels(exclude: string[] = []) {
  const { preferences, isLoaded } = usePreferences();
  const { models: visibleModels, modelAccessMap, validModelNames } = useAllModels();
  const { mutate } = useUpdatePreferences();
  const defaults = useModeDefaultModels();

  // starred_models stayed in other_preference when model routing moved to
  // its own column, so the defaults below are read from a different place.
  const other = (preferences as PreferencesLike | null)?.other_preference ?? {};
  const starred = modelList(other[STARRED_KEY]);
  const hidden = modelList(other[HIDDEN_KEY]);

  const models = deriveQuickAccessModels({
    // Each mode's default as the composer resolves it, the deployment's when
    // nothing is saved: a pick on a thread saves no preference, so this row
    // can be the only way back to a default nobody chose.
    preferredModel: defaults.ptc,
    preferredFlashModel: defaults.fast,
    starredModels: starred,
    // On a platform the access map is what says which models are the user's
    // own; until it loads (or if it fails) none are known, rather than every
    // listed model the way a deployment without a platform reads them.
    ownModels: isPlatformMode && !modelAccessMap ? [] : ownModelNames(visibleModels, modelAccessMap),
    hiddenModels: hidden,
    validModelNames,
    excludeModels: exclude,
  });

  // Each edit replaces both lists whole, so one built on preferences not yet
  // read would erase the saved ones; it waits for the read instead.
  const write = useCallback((nextStarred: string[], nextHidden: string[]) => {
    if (!isLoaded) return;
    mutate(splitPreferenceWrite({
      [STARRED_KEY]: nextStarred.length > 0 ? nextStarred : null,
      [HIDDEN_KEY]: nextHidden.length > 0 ? nextHidden : null,
    }));
  }, [isLoaded, mutate]);

  const add = useCallback((model: string) => write(
    starred.includes(model) ? starred : [...starred, model],
    hidden.filter((m) => m !== model),
  ), [starred, hidden, write]);

  const remove = useCallback((model: string) => write(
    starred.filter((m) => m !== model),
    hidden.includes(model) ? hidden : [...hidden, model],
  ), [starred, hidden, write]);

  return { models, starred, add, remove };
}
