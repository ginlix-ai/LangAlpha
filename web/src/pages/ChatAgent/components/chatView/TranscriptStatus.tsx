import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Clock, Info } from 'lucide-react';
import { HoverCard, HoverCardTrigger, HoverCardContent } from '@/components/ui/hover-card';
import { Loader } from '@/components/ui/loader';
import { AnimatePresence, motion, useReducedMotion } from '@/lib/framer';
import { compactNumber } from '@/lib/format';
import { DURATION, EASE_COLLAPSE, TWEEN } from '@/lib/motion';
import { useLocale } from '@/hooks/useLocale';
import { useModelName } from '@/hooks/useModelName';
import type { ModelStatus, TurnStatus } from '../../session/types';
import type { RetiredModel } from '../../hooks/useThreadModel';
import { useWaitingAutomations } from '../../../Automations/hooks/useWaitingAutomations';
import { QueuedAutomationNotice } from '../QueuedAutomationNotice';
import { CliQueueLine } from '../CliQueueNotice';
import { ModelStatusPill } from './ModelStatusPill';
import { RetiredModelNotice } from './RetiredModelNotice';
import { StatusLine } from './StatusLine';

/** A row with a surface of its own under the lines (the thread's error, its
 *  computer's warnings), which decides on its own whether it shows. */
export interface ThreadNotice {
  key: string;
  node: React.ReactNode;
}

/** Finds the block TranscriptStatus renders (its `data-transcript-status`). */
export const TRANSCRIPT_STATUS_SELECTOR = '[data-transcript-status]';

interface TranscriptStatusProps {
  modelStatus: ModelStatus | null;
  isLoading: boolean;
  retired: RetiredModel | null;
  /** What the turn waits on, its workspace's start merged with the warm
   *  the page itself watches. */
  turnStatus: TurnStatus;
  /** The model the thread runs, which a hand-off's wait names. */
  model: string | null;
  isCompacting: string | false;
  /** About how many tokens the summary being written has written. */
  compactionTokens: number | null;
  queuedSend: string | false;
  /** The thread whose due automations wait for its turn to end. */
  threadId: string;
  active: boolean;
  /** In the block so a reader at the end sees one arrive as they see a line. */
  notices?: readonly ThreadNotice[];
  /** The notices went to the docked composer: they leave here at once, since
   *  closing would keep a second copy mounted (and a warning's alert read out
   *  twice) while the composer already shows them. */
  noticesDocked?: boolean;
}

/* The thread's status lines, under the last reply and in the transcript's
   flow. Above the composer they floated over whatever scrolled beneath them
   and read as part of it. The block is always mounted: useChatScroll watches
   it so a reader at the end sees a line arrive. The gap above it belongs to
   its first line, so with nothing to say it takes no room.
   Reconnecting is not one of them: it floats over the composer instead
   (ReconnectingChip), since a reconnect that catches up would take its line
   out from under a reader at the end and drop the reply by its height.

   A line fades in, and leaves by fading as its height closes, gap included:
   under a reader at the end, a line that vanished at once would drop the
   reply by its height in one frame. Lines already showing when the transcript
   mounts (a thread opening, a tab coming back) are simply there.
   A notice leaves the same way when the host takes it away (an error
   cleared, the workspace's computer gone). One that hides itself (dismissed,
   or its condition read as cleared) still goes at once: only it knows, and
   the block sees no more than an empty row. */
export function TranscriptStatus({
  modelStatus,
  isLoading,
  retired,
  turnStatus: { workspaceStarting, turnHeld, cliQueue },
  model,
  isCompacting,
  compactionTokens,
  queuedSend,
  threadId,
  active,
  notices = [],
  noticesDocked = false,
}: TranscriptStatusProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const reducedMotion = useReducedMotion();
  const lines: { key: string; node: React.ReactNode }[] = [];
  if (modelStatus && isLoading) {
    lines.push({ key: 'model', node: <ModelStatusPill modelStatus={modelStatus} isLoading={isLoading} /> });
  }
  if (retired) lines.push({ key: 'retired', node: <RetiredModelNotice {...retired} /> });
  if (workspaceStarting) {
    lines.push({ key: 'workspace', node: <WorkspaceStartingLine archived={workspaceStarting === 'archived'} /> });
  }
  if (turnHeld !== null) lines.push({ key: 'turnHeld', node: <TurnHeldLine reason={turnHeld} model={model} /> });
  if (cliQueue) lines.push({ key: 'cliQueue', node: <CliQueueLine wait={cliQueue} /> });
  if (isCompacting) {
    lines.push({
      key: 'compacting',
      node: (
        <div className="flex items-center gap-2">
          <StatusLine glyph={<Loader size={14} className="text-(--color-accent-primary)" />}>
            {t(isCompacting === 'offload' ? 'chat.offloading' : 'chat.compacting')}
          </StatusLine>
          {/* Beside the live region rather than in it, which would read the
              count out every time it moves; a screen reader still reaches it. */}
          {isCompacting === 'summarize' && compactionTokens != null && (
            <span className="text-xs tabular-nums" style={{ color: 'var(--color-text-quaternary)' }}>
              {t('chat.compactingTokens', { value: compactNumber(compactionTokens, locale) })}
            </span>
          )}
        </div>
      ),
    });
  }
  if (queuedSend) {
    lines.push({
      key: 'queued',
      node: (
        <StatusLine
          glyph={<Clock className="h-3.5 w-3.5" style={{ color: 'var(--color-accent-primary)' }} />}
          title={queuedSend === '…' ? undefined : queuedSend}
        >
          {t('chat.queuedSend')}
        </StatusLine>
      ),
    });
  }
  const waiting = useWaitingAutomations(threadId, active);
  if (waiting.length) lines.push({ key: 'automations', node: <QueuedAutomationNotice runs={waiting} /> });
  const exit = reducedMotion ? { duration: 0 } : { duration: DURATION.exit, ease: EASE_COLLAPSE };
  const row = {
    className: 'overflow-hidden',
    initial: { opacity: 0 },
    animate: { opacity: 1, transition: reducedMotion ? { duration: 0 } : TWEEN.quick },
    exit: { opacity: 0, height: 0, transition: exit },
  };
  // A leaving child has no props left to read, so whether it closes or goes
  // at once reaches it through AnimatePresence's `custom`.
  const noticeRow = {
    ...row,
    variants: {
      gone: (docked: boolean) => ({ opacity: 0, height: 0, transition: docked ? { duration: 0 } : exit }),
    },
    exit: 'gone',
  };
  return (
    <div data-transcript-status className="flex flex-col">
      <AnimatePresence initial={false} custom={noticesDocked}>
        {/* Its own box rather than the first line's, which a line leaving
            from the top would hand to the next one in a frame while its own
            was still closing. It closes with the block's last line. */}
        {lines.length > 0 && (
          <motion.div key="gap" aria-hidden className="h-3" exit={{ height: 0, transition: exit }} />
        )}
        {lines.map(({ key, node }) => (
          <motion.div key={key} {...row}>
            {node}
          </motion.div>
        ))}
        {/* Each notice's gap is inside its own closing box, and hidden with
            it while the notice renders nothing. */}
        {notices.map(({ key, node }) => (
          <motion.div key={`notice:${key}`} {...noticeRow} custom={noticesDocked}>
            <div className="flex flex-col gap-3 pt-3 empty:hidden">{node}</div>
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}

function turnHeldKey(reason: string): string {
  if (reason === 'compact') return 'chat.compacting';
  if (reason === 'handoff') return 'chat.turnHeld.handoff';
  return 'chat.turnHeld.other';
}

/** The wait's line. A hand-off names the model it goes to, never who writes
 *  the brief, which is settled only once one is written. */
function TurnHeldLine({ reason, model }: { reason: string; model: string | null }): React.ReactElement {
  const { t } = useTranslation();
  const name = useModelName();
  const text = reason !== 'brief'
    ? t(turnHeldKey(reason))
    : model
      ? t('chat.turnHeld.brief', { model: name(model) })
      : t('chat.turnHeld.briefNoModel');
  return (
    <StatusLine glyph={<Loader size={12} className="text-(--color-accent-primary)" />}>
      {text}
    </StatusLine>
  );
}

function WorkspaceStartingLine({ archived }: { archived: boolean }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <StatusLine live={false} glyph={<Loader size={12} className="text-(--color-accent-primary)" />}>
      <span>{t(archived ? 'chat.workspaceRestoring' : 'chat.workspaceStarting')}</span>
      <HoverCard openDelay={150} closeDelay={100}>
        <HoverCardTrigger asChild>
          <button
            type="button"
            aria-label={t('chat.workspaceStateHelp')}
            className="inline-flex items-center justify-center rounded-full p-0.5 hover:opacity-80 focus:outline-hidden focus-visible:ring-1 focus-visible:ring-ring"
            style={{ color: 'var(--color-text-quaternary)' }}
          >
            <Info className="h-3 w-3" />
          </button>
        </HoverCardTrigger>
        <HoverCardContent side="top" align="start" className="w-80 text-xs leading-relaxed">
          <div className="font-medium mb-1" style={{ color: 'var(--color-text-primary)' }}>
            {t(archived ? 'chat.workspaceStateArchivedTitle' : 'chat.workspaceStateStartingTitle')}
          </div>
          <p style={{ color: 'var(--color-text-secondary)' }}>
            {t(archived ? 'chat.workspaceStateArchivedBody' : 'chat.workspaceStateStartingBody')}
          </p>
          {archived && (
            <p className="mt-2" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('chat.workspaceStateArchivedFootnote')}
            </p>
          )}
        </HoverCardContent>
      </HoverCard>
    </StatusLine>
  );
}
