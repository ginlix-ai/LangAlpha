import React, { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { SwapCell } from '@/components/ui/crossfade';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { cn } from '@/lib/utils';
import { formatClock } from '@/lib/elapsed';
import { SubagentGlyph } from '../../mcp/subagentGlyph';
import { SUBAGENT_ROW_UI, type SubagentRowKind } from '../../taskStatusUi';
import {
  RunAvatar,
  RUN_CLOCK,
  RUN_DETAIL,
  RUN_FOCUS,
  RUN_HOVER,
  RUN_ROW,
  RUN_TITLE,
  RunPart,
  RunStatus,
} from '../runCard';
import type { SubagentInfo } from '../types';
import { memberElapsed, type SubagentRowView } from './model';
import { RunModelPart } from '../RunModelPart';
import { useSteadyStep } from './useSteadyStep';

/** Where a subagent row stands, leading the line under its title. The done
 *  state has no glyph, so a settled group reads as words and only what went
 *  wrong stands out. */
function SubagentRowStatus({ id, kind, rawStatus }: {
  id?: string;
  kind: SubagentRowKind;
  rawStatus?: string;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const { labelKey, glyph, danger } = SUBAGENT_ROW_UI[kind];
  const words = labelKey ? t(labelKey) : rawStatus;
  if (!words) return null;
  return <RunStatus id={id} data-testid="subagent-row-status" glyph={glyph} words={words} danger={danger} />;
}

/**
 * One subagent, alone or in a group: its role, what it was asked, then where
 * it stands, the model it runs on, and what it is doing, what it returned or
 * why it failed, and its time on the right.
 *
 * It opens the subagent when it has one to open and the surface allows it.
 * Otherwise (a shared link, a launch that never ran) it renders inert: no
 * button and no focus stop, so nothing promises a navigation that cannot
 * happen. It draws no chevron either way: a chevron in a reply unfolds in
 * place, and this row goes elsewhere.
 */
export function SubagentRow({
  view,
  now,
  onOpen,
}: {
  view: SubagentRowView;
  now: number;
  onOpen?: (info: SubagentInfo) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const detailId = useId();
  const statusId = useId();
  const modelId = useId();
  const elapsedId = useId();
  const elapsed = memberElapsed(view, now);
  // How long it took is part of what a settled row says; a clock still
  // ticking is left out, as `ElapsedClock` leaves it, or it would be heard
  // every second.
  const spokenElapsed = elapsed != null && !view.live;
  const info = view.openInfo;
  const open = onOpen && info ? () => onOpen(info) : undefined;
  const { model } = view;
  // A working run's step is steadied, so its calls read as steps; whatever
  // else the line says (a result, a reason) is shown as it comes.
  const working = view.kind === 'working' || view.kind === 'pausing';
  const { text: detail, between } = useSteadyStep(view.detail, working);
  // The calls running beside the newest, counted live while the line names a
  // call, even one still giving way to the newest, so a swap never blinks it.
  const more = working && !between ? view.detail.others ?? 0 : 0;

  const body = (
    <>
      <RunAvatar>
        <SubagentGlyph type={view.type} className="h-3.5 w-3.5 stroke-[1.75]" />
      </RunAvatar>
      <span className="min-w-0">
        <span className={cn(RUN_TITLE, 'block')} title={view.fullTitle || undefined}>{view.title}</span>
        <span className={RUN_DETAIL}>
          <SubagentRowStatus id={statusId} kind={view.kind} rawStatus={view.rawStatus} />
          {/* Before the detail, so it holds still while the detail changes. */}
          {model && <RunModelPart id={modelId} model={model} />}
          {/* Each new step crossfades in place and, while the run works,
              shimmers as a running tool call's title does. It does not rise as
              the status line's does: on a row's small second line, with
              several rows stepping at once, the travel read as a hop. A
              leaving layer is hidden from a screen reader, which hears the
              step from the one span below instead. */}
          <SwapCell swapKey={detail || null} layerClassName="flex min-w-0 items-center gap-1.5">
            <RunPart>
              <span aria-hidden data-testid="subagent-row-detail" className="flex min-w-0 items-center gap-1.5" title={detail}>
                {working ? (
                  <TextShimmer
                    as="span"
                    className="min-w-0 truncate [--base-color:var(--color-text-secondary)] [--base-gradient-color:var(--color-text-primary)]"
                    duration={1.5}
                  >
                    {detail}
                  </TextShimmer>
                ) : (
                  <span className="min-w-0 truncate">{detail}</span>
                )}
                {/* Updated in place: a count changing is not a new step. */}
                {more > 0 && (
                  <span data-testid="subagent-row-more" className="shrink-0 text-(--color-text-tertiary)">
                    {t('toolArtifact:moreRunningShort', { count: more })}
                  </span>
                )}
              </span>
            </RunPart>
          </SwapCell>
          {detail && (
            <span id={detailId} className="sr-only">
              {more > 0 ? t('chat.subagentRun.stepWithMore', { step: detail, count: more }) : detail}
            </span>
          )}
        </span>
      </span>
      {elapsed != null && (
        <span id={elapsedId} data-testid="subagent-row-elapsed" className={RUN_CLOCK}>{formatClock(elapsed)}</span>
      )}
    </>
  );

  if (!open) {
    return <div data-testid="subagent-row" className={RUN_ROW}>{body}</div>;
  }
  return (
    <button
      type="button"
      data-testid="subagent-row"
      aria-label={t('chat.subagentRun.openLabel', { title: view.title })}
      aria-describedby={[
        statusId,
        model ? modelId : null,
        detail ? detailId : null,
        spokenElapsed ? elapsedId : null,
      ].filter(Boolean).join(' ')}
      onClick={open}
      className={cn(RUN_ROW, RUN_HOVER, RUN_FOCUS)}
    >
      {body}
    </button>
  );
}
