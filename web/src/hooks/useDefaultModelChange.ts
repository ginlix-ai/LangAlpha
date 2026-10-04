import { useCallback, useState } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import {
  defaultModelKey,
  readDefaultModelScope,
  splitPreferenceWrite,
  threadsMoved,
  type ApplyDefaultTo,
  type ComposerMode,
  type FlashFollows,
  type PreferencePatch,
} from '@/lib/modelPreferences';
import { useModeDefaultModels } from './useModeDefaultModel';
import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

/** What each default becomes: fast names the flash default, PTC the primary one. */
type DefaultModelChoice = Partial<Record<ComposerMode, string>>;

/** The question a parked change asks, as `DefaultModelScopeChoice` takes it. */
export interface DefaultModelQuestion {
  /** The new defaults, as the calling surface prints models: one, or the
   *  primary and flash pair when Settings changes both. */
  models: string[];
  /** The defaults being replaced, printed the same way; empty when unknown. */
  previous: string[];
  saving: boolean;
  onConfirm: (applyTo: ApplyDefaultTo, remember: boolean) => void;
  onCancel: () => void;
}

const MODES: ComposerMode[] = ['ptc', 'fast'];

interface Draft {
  /** Every default the answer writes, including any that already resolve to
   *  the model named. */
  models: DefaultModelChoice;
  /** The default each changing mode replaces, which names the threads the
   *  change can move. Only modes whose default actually changes appear. */
  previous: Partial<Record<ComposerMode, string | null>>;
}

/** One model, or a pair joined as "A and B", with the i18next `context` that
 *  picks a message's wording for a pair. */
export function namePhrase(t: TFunction, names: string[]): { text: string; context: 'pair' | undefined } {
  return names.length > 1
    ? { text: t('settings.defaultModelChange.modelPair', { first: names[0], second: names[1] }), context: 'pair' }
    : { text: names[0] ?? '', context: undefined };
}

/** A draft as the question and the toast print it: the new models and the
 *  ones they replace, each named once, primary first. A draft that changes no
 *  default names what it writes. */
function describe(draft: Draft, label: (model: string) => string): { models: string[]; previous: string[] } {
  const changing = MODES.filter((mode) => mode in draft.previous);
  const models = new Set<string>();
  const previous = new Set<string>();
  for (const mode of changing.length > 0 ? changing : MODES) {
    const model = draft.models[mode];
    if (model) models.add(label(model));
    const replaced = draft.previous[mode];
    if (replaced) previous.add(label(replaced));
  }
  return { models: [...models], previous: [...previous] };
}

/**
 * Changing the account default models, and the question that raises about the
 * threads already running on the old ones.
 *
 * One flow for each surface in this client that changes a default (the
 * thread banner and Settings). With no saved answer, `request` parks the
 * change as a draft and hands back the `question` for the caller to render
 * as `DefaultModelScopeChoice`, and a further request while it is parked
 * joins it, so Settings can change both defaults under one question without
 * the second pick discarding the first. With a saved answer the change
 * applies at once. Either way the server moves only threads still on the old
 * default, so a thread given a model on purpose keeps it.
 *
 * `label` prints a model the way the calling surface does, so the toast names
 * it as the control beside it does.
 */
export function useDefaultModelChange(label: (model: string) => string) {
  const { t } = useTranslation();
  const { preferences } = usePreferences();
  const defaults = useModeDefaultModels();
  const { mutate, mutateAsync, isPending: saving } = useUpdatePreferences();
  const [pending, setPending] = useState<Draft | null>(null);

  // `answers` is the parked draft this write settles, if any.
  const apply = useCallback(async (
    change: Draft,
    applyTo: ApplyDefaultTo,
    remember: boolean,
    answers: Draft | null,
  ) => {
    // Every default goes in one write, so the server moves each mode's
    // threads against the final pair rather than a half-applied one.
    // Remembering the answer saves it as the scope too; `apply_default_to`
    // is a one-shot answer for this write and is never stored.
    // A saved flash model makes Auto moot, so the pick drops it rather than
    // leave it to resurface if the model is ever cleared.
    const patch: PreferencePatch = {};
    for (const mode of MODES) {
      const model = change.models[mode];
      if (model) patch[defaultModelKey(mode)] = model;
    }
    if (change.models.fast) patch.flash_follows = null;
    if (remember) patch.default_model_scope = applyTo;
    let result: Record<string, unknown> | undefined;
    try {
      result = await mutateAsync({ ...splitPreferenceWrite(patch), apply_default_to: applyTo });
    } catch {
      toast({ description: t('settings.defaultModelChange.failed'), variant: 'destructive' });
      return;
    }
    // A pick made while this write was in flight joined the draft as a new
    // one, which is still waiting for its own answer.
    setPending((current) => (current === answers ? null : current));

    const moved = threadsMoved(result);
    const names = describe(change, label);
    const model = namePhrase(t, names.models);
    const previous = namePhrase(t, names.previous);
    toast({
      description: applyTo === 'new_threads'
        ? t('settings.defaultModelChange.appliedNew', { model: model.text, context: model.context })
        : moved !== undefined && moved > 0
          ? t('settings.defaultModelChange.appliedExisting', { model: model.text, count: moved, context: model.context })
          : moved === 0 && names.previous.length > 0
            ? t('settings.defaultModelChange.appliedExistingNone', {
              model: model.text,
              previous: previous.text,
              context: model.context,
            })
            : t('settings.defaultModelChange.applied', { model: model.text, context: model.context }),
    });
  }, [mutateAsync, label, t]);

  const request = useCallback((models: DefaultModelChoice) => {
    const draft: Draft = { models: { ...pending?.models, ...models }, previous: {} };
    for (const mode of MODES) {
      const model = draft.models[mode];
      if (model && model !== defaults[mode]) draft.previous[mode] = defaults[mode];
    }
    // Naming the models the defaults already resolve to (the primary model a
    // flash default inherits, say) moves no thread, so there is nothing to ask.
    const scope = Object.keys(draft.previous).length === 0 ? 'new_threads' : readDefaultModelScope(preferences);
    if (scope === 'ask') setPending(draft);
    else void apply(draft, scope, false, pending);
  }, [pending, defaults, preferences, apply]);

  /** Clearing a default skips the question: the server still applies a saved
   *  answer to it, and with none moves no thread. A cleared primary runs the
   *  deployment's model; a cleared flash runs the primary, or with `follows`
   *  of `deployment` (Auto) the deployment's flash model. The cleared mode
   *  leaves the draft, so its control shows the clear rather than the pick it
   *  replaces. */
  const clear = useCallback((mode: ComposerMode, follows: FlashFollows = 'primary') => {
    mutate(splitPreferenceWrite(mode === 'fast'
      ? { preferred_flash_model: null, flash_follows: follows === 'deployment' ? follows : null }
      : { preferred_model: null }));
    setPending((current) => {
      if (!current || !(mode in current.models)) return current;
      const models = { ...current.models };
      const previous = { ...current.previous };
      delete models[mode];
      delete previous[mode];
      return Object.keys(models).length > 0 ? { models, previous } : null;
    });
  }, [mutate]);

  const confirm = useCallback((applyTo: ApplyDefaultTo, remember: boolean) => {
    if (pending) void apply(pending, applyTo, remember, pending);
  }, [pending, apply]);

  const cancel = useCallback(() => setPending(null), []);

  const question: DefaultModelQuestion | null = pending
    ? { ...describe(pending, label), saving, onConfirm: confirm, onCancel: cancel }
    : null;

  return { question, draft: pending?.models ?? null, saving, request, clear };
}
