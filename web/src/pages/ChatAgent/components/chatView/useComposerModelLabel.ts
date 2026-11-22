import { useCallback } from 'react';
import { getModelDisplayName } from '@/components/ui/chat-input.helpers';
import { useAllModels } from '@/hooks/useAllModels';

/** Prints a model the way the composer pill does, so a notice names it as
 *  the control it sits on. */
export function useComposerModelLabel(): (model: string) => string {
  const { metadata } = useAllModels();
  return useCallback((model: string) => getModelDisplayName(model, metadata), [metadata]);
}
