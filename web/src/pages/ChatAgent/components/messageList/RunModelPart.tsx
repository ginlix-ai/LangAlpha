import React from 'react';
import { useTranslation } from 'react-i18next';
import { ModelMark } from '@/components/model/ModelMark';
import { getModelDisplayName } from '@/components/ui/chat-input.helpers';
import { useModelMetadata } from '@/hooks/useModelName';
import { effortLabelFor } from '@/lib/effortLabels';
import { llmProviderArt, nameBesideMark } from '@/lib/providerArt';
import { cn } from '@/lib/utils';
import type { RunModel } from '@/types/chat';
import { RunPart } from './runCard';

/**
 * What a run went out on, as a part of its second line: the provider's mark,
 * the name without the brand word the mark stands for, and the effort in the
 * status's quieter ink, as the composer's model pill draws them. A provider
 * we ship no art for keeps its whole name. It holds its width, so whatever
 * follows it on the line is what gets cut.
 *
 * `yields`, it gives way to what comes before it once its line's container
 * is narrow (`@container`, below `sm`): a model with a mark keeps only the
 * mark, as the composer's pill does at its floor, and one without keeps its
 * name, cut before anything else on the line is.
 *
 * The drawn part is hidden from a screen reader, which hears the full name
 * and the effort as an effort from `id` instead, however much is drawn.
 */
export function RunModelPart({
  model,
  id,
  yields = false,
}: {
  model: RunModel;
  id?: string;
  yields?: boolean;
}): React.ReactElement {
  const { t } = useTranslation();
  const metadata = useModelMetadata();
  const name = getModelDisplayName(model.id, metadata);
  // The mark the model menu draws for this model: the catalog's own provider
  // (a Claude Code row's is Claude Code's, not the API's), else the run's.
  const provider = metadata?.[model.id]?.provider ?? model.provider;
  const { shown } = nameBesideMark(name, provider);
  const effort = effortLabelFor(t, model.effort ?? null);
  const marked = !!llmProviderArt(provider);
  const cut = yields && !marked;
  return (
    <>
      <RunPart>
        <span
          aria-hidden
          data-testid="run-model"
          className={cn(
            'inline-flex items-center gap-1 whitespace-nowrap',
            // Far more eager to shrink than the workspace before it.
            cut ? 'min-w-0 shrink-[8]' : 'shrink-0',
          )}
        >
          <ModelMark provider={provider} className="h-3 w-3" />
          <span className={cn('inline-flex min-w-0 items-center gap-1', yields && marked && '@max-sm:hidden')}>
            <span className={cn(cut && 'min-w-0 truncate')}>{shown}</span>
            {effort && (
              <span className={cn('shrink-0 text-(--color-text-tertiary)', yields && '@max-sm:hidden')}>{effort}</span>
            )}
          </span>
        </span>
      </RunPart>
      <span id={id} className="sr-only">
        {effort ? t('chat.runModel.withEffort', { model: name, effort }) : name}
      </span>
    </>
  );
}
