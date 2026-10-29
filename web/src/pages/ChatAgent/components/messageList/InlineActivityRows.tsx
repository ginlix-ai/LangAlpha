import React, { memo } from 'react';
import { motion } from '@/lib/framer';
import { Wrench } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { DotLoader } from '@/components/ui/dot-loader';
import { ToolIcon } from '../ToolIcon';
import { getDisplayName, getToolIcon, getPreparingText } from '../toolDisplayConfig';
import { liveToolLabel, completedToolTitle, completedToolSummary } from './activitySummary';
import { TWEEN } from '@/lib/motion';
import { FailedIconBadge, RunningIcon } from './ActivityRowParts';
import { SwapCell } from '@/components/ui/crossfade';
import type { ToolActivityItem, LiveState, PreparingCall } from './activityTypes';

const DIM = TWEEN.quick;

interface ToolCallLiveRowProps {
  tc: ToolActivityItem;
  liveState?: LiveState;
}

/** Live tool call row -- monochrome state visuals.
 *
 * Active state:    2px left rule (.nrow.state-active::before) + label shimmer
 *                  + gentle icon pulse.
 * Completing state: no badge, row dims via opacity 0.7 and the title
 *                  crossfades to past tense. (We deliberately dropped the
 *                  green ✓ to avoid the SaaS-default badge look.)
 * Failed state:    gray ✕ badge overlaid on the tool icon + past-tense title.
 */
export const ToolCallLiveRow = memo(function ToolCallLiveRow({ tc, liveState }: ToolCallLiveRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const args = tc.toolCall?.args;
  const isInProgress = liveState === 'active' && !tc.isComplete && !tc._recentlyCompleted;
  // Only `state-active` has a CSS treatment (left-rule shimmer in
  // ActivityBlock.css). Completing and failed states get their visual cue
  // from the inline icon badges below; keeping unused class hooks would
  // mislead the next CSS author.
  const stateClass = isInProgress ? 'state-active' : '';

  const activeLabel = isInProgress ? liveToolLabel(tc, t) : null;
  const artifact = tc.toolCallResult?.artifact;
  const completedTitle = !isInProgress ? completedToolTitle(tc, t) : null;
  const summary = !isInProgress ? completedToolSummary(tc, t) : null;

  return (
    <motion.div
      className={`nrow ${stateClass} flex items-center gap-2 pl-3 pr-3 py-1.5`}
      animate={{ opacity: isInProgress ? 1 : 0.7 }}
      transition={DIM}
      style={{ fontSize: '0.8125rem', color: 'var(--Labels-Secondary)' }}
    >
      <div className="relative shrink-0 flex items-center justify-center h-5 w-5">
        <RunningIcon running={isInProgress}>
          <ToolIcon toolName={toolName} args={args} artifact={artifact} className="h-4 w-4" />
        </RunningIcon>
        <FailedIconBadge failed={liveState === 'failed'} label={t('toolArtifact.a11y.toolCallFailed')} />
      </div>
      <SwapCell swapKey={isInProgress ? 'running' : 'settled'} className="flex-1" layerClassName="flex items-center gap-2">
        {isInProgress ? (
          <TextShimmer
            as="span"
            className="font-medium text-[0.8125rem] [--base-color:var(--Labels-Secondary)] [--base-gradient-color:var(--color-text-primary)] truncate"
            duration={1.5}
          >
            {activeLabel || ''}
          </TextShimmer>
        ) : (
          <>
            <span className="font-medium shrink-0 whitespace-nowrap">{completedTitle}</span>
            {summary
              ? <span className="truncate min-w-0" style={{ opacity: 0.55 }}>&mdash; {summary}</span>
              : <span style={{ opacity: 0.55 }}>{t('toolArtifact.done')}</span>}
          </>
        )}
      </SwapCell>
    </motion.div>
  );
});

interface PreparingToolCallRowProps {
  tc: PreparingCall;
}

/** Preparing row -- shown while tool_call_chunks are still streaming.
 *  No left rule; just DotLoader + icon + label. Args aren't yet available
 *  to classify, so we fall back to the generic display name. */
export function PreparingToolCallRow({ tc }: PreparingToolCallRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const displayName = toolName ? getDisplayName(toolName, t) : t('toolArtifact.toolCall');
  const IconComponent: LucideIcon = toolName ? getToolIcon(toolName) : Wrench;
  const prepText = getPreparingText(toolName, tc.argsLength, t);

  return (
    <div
      className="nrow flex items-center gap-2 pl-3 pr-3"
      style={{
        fontSize: '0.8125rem',
        color: 'var(--Labels-Secondary)',
        padding: '6px 12px',
        opacity: 0.85,
      }}
    >
      <DotLoader
        className="shrink-0 gap-px"
        dotClassName="bg-foreground/15 [&.active]:bg-foreground size-[1.5px]"
      />
      <span className="shrink-0 flex items-center justify-center h-5 w-5">
        <IconComponent className="h-4 w-4" />
      </span>
      <span className="font-medium">{displayName}</span>
      <span style={{ opacity: 0.55 }}>{prepText}</span>
    </div>
  );
}
