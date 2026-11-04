import { useAllModels } from './useAllModels';
import { isClaudeCodeModel } from '@/lib/claudeCode';
import { useDefaultModel } from './useSlotDefaultModels';

/**
 * The models a composer offers for where its send goes, and the model it
 * starts on.
 *
 * A turn in Home is the Chief of Staff's, which never runs Claude Code: the
 * server reads a saved Claude Code Default there as unset and refuses one a
 * send names, so such a model, saved, held or picked for a workspace, gives
 * way to the deployment's Default.
 */
export function useTurnModels(home: boolean) {
  const { models, slotModels, metadata, systemDefaults } = useAllModels();
  const accountDefault = useDefaultModel();
  const offers = (m: string) => !home || !isClaudeCodeModel(metadata[m]);
  const defaultModel = accountDefault && !offers(accountDefault) ? systemDefaults?.default_model ?? null : accountDefault;
  return {
    models: home ? slotModels : models,
    defaultModel,
    offers,
    resolve: (m: string | null) => (m && !offers(m) ? defaultModel : m),
  };
}
