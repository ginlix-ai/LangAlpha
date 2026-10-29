import React, { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { formatClock } from '@/lib/elapsed';
import { SUBAGENT_ROW_UI, type SubagentRowKind } from '../../taskStatusUi';
import {
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
import { ModelWords, SubagentTile } from './SubagentTile';

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
 * it stands with what it is doing, what it returned or why it failed, and its
 * time on the right.
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

  const body = (
    <>
      <SubagentTile type={view.type} model={model} />
      <span className="min-w-0">
        <span className={cn(RUN_TITLE, 'block')} title={view.fullTitle || undefined}>{view.title}</span>
        <span className={RUN_DETAIL}>
          <SubagentRowStatus id={statusId} kind={view.kind} rawStatus={view.rawStatus} />
          {view.detail && (
            <RunPart>
              <span id={detailId} data-testid="subagent-row-detail" className="min-w-0 truncate" title={view.detail}>
                {view.detail}
              </span>
            </RunPart>
          )}
        </span>
      </span>
      {elapsed != null && (
        <span id={elapsedId} data-testid="subagent-row-elapsed" className={RUN_CLOCK}>{formatClock(elapsed)}</span>
      )}
      {model && <ModelWords id={modelId} model={model} />}
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
        view.detail ? detailId : null,
        spokenElapsed ? elapsedId : null,
        model ? modelId : null,
      ].filter(Boolean).join(' ')}
      onClick={open}
      className={cn(RUN_ROW, RUN_HOVER, RUN_FOCUS)}
    >
      {body}
    </button>
  );
}
