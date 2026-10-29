import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useModelName } from '@/hooks/useModelName';
import { llmProviderArt } from '@/lib/brandArt';
import type { SubagentModel } from '../../../session/subagents/taskRuns';
import { SubagentGlyph } from '../../mcp/subagentGlyph';
import { RunAvatar } from '../runCard';

/**
 * The provider's mark on the avatar's lower right, cut out of it by a ring in
 * the row's color and then the hairline. The row's hover fill is a wash over
 * the page, so on hover the ring takes the same wash over its page color. The
 * bed stays light in both themes, which is also what a logo drawn as a dark
 * glyph on transparency needs.
 */
function ModelBadge({ model }: { model: SubagentModel }): React.ReactElement | null {
  const label = useModelName()(model.id);
  const art = llmProviderArt(model.provider);
  // Keyed by src, so other art re-arms the load rather than inheriting a failure.
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  if (!art || failedSrc === art.src) return null;
  return (
    <span
      data-testid="subagent-model-badge"
      title={label}
      className="absolute -right-1 -bottom-1 flex h-[13px] w-[13px] items-center justify-center rounded-full bg-white shadow-[0_0_0_1.5px_var(--color-bg-page),0_0_0_2.5px_var(--color-border-default)] group-hover/run:shadow-[0_0_0_1.5px_var(--color-bg-hover),0_0_0_1.5px_var(--color-bg-page),0_0_0_2.5px_var(--color-border-default)]"
    >
      <img
        src={art.src}
        alt=""
        className="h-[9px] w-[9px] object-contain"
        onError={() => setFailedSrc(art.src)}
      />
    </span>
  );
}

/** A subagent's role on its avatar, badged with its model when that is news. */
export function SubagentTile({
  type,
  model,
}: {
  type: string;
  model?: SubagentModel;
}): React.ReactElement {
  return (
    <RunAvatar badge={model ? <ModelBadge model={model} /> : undefined}>
      <SubagentGlyph type={type} className="h-3.5 w-3.5 stroke-[1.75]" />
    </RunAvatar>
  );
}

/** The model in words, for a screen reader: the badge itself is decoration. */
export function ModelWords({ id, model }: { id: string; model: SubagentModel }): React.ReactElement {
  const { t } = useTranslation();
  const label = useModelName()(model.id);
  return <span id={id} className="sr-only">{t('chat.subagentRun.model', { model: label })}</span>;
}
