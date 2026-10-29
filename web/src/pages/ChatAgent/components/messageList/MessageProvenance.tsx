/**
 * The caption above a message nobody typed in this view, naming who sent it:
 * an agent's hand-off, an automation, a channel, a subagent's main agent. A
 * report-back is drawn instead as a line of its own (`ReportRow`).
 *
 * Where the sender has a place in the app the caption links there. Read-only
 * views draw it without the link, as they draw a card's.
 */
import React from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight, Bot, Clock, CornerDownRight, Merge, MessageCircle, type LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { MessageProvenance } from '@/types/chat';
import { useRouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';
import { Favicon } from '../Favicon';
import { messagingAppDomain, messagingAppName } from '../messaging/messageDelivery';
import { useMessageActions } from './MessageActionsContext';

export const CAPTION = 'inline-flex max-w-full min-w-0 items-center gap-1.5 text-xs leading-4 text-(--color-text-tertiary)';
// The padding gives the hover fill room, and the negative margins and the shift
// take it back so a linked caption sits exactly where a plain one does: its
// text on the bubble's edge, its line no taller. The shift is a translate, not
// a negative margin: the caption sits in a box sized to it, and a negative
// right margin would shrink that box below the text and truncate it.
const LINKED = cn(
  CAPTION,
  '-my-0.5 translate-x-1.5 cursor-pointer rounded-md px-1.5 py-0.5 text-left outline-hidden transition-colors',
  'hover:bg-(--color-bg-hover) hover:text-(--color-text-secondary) focus-visible:ring-2 focus-visible:ring-ring',
);

interface MessageProvenanceProps {
  provenance: MessageProvenance;
  readOnly?: boolean;
  className?: string;
}

export function MessageProvenanceLabel({ provenance, readOnly, className }: MessageProvenanceProps): React.ReactElement | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  const { onOpenDispatcher } = useMessageActions();

  let Icon: LucideIcon | null = null;
  let logo: React.ReactNode = null;
  let label: React.ReactNode;
  let title: string | undefined;
  let open: (() => void) | null = null;

  switch (provenance.kind) {
    case 'agent': {
      // A hand-off with no recorded dispatcher stays unlabelled in a share,
      // as shares have always shown it.
      if (!provenance.agent && readOnly) return null;
      Icon = Bot;
      // Only the Chief of Staff hands turns out. A row recorded before it named
      // its dispatcher reads the same, as the thread's back button does.
      label = t('chat.provenance.fromChiefOfStaff');
      const { threadId } = provenance;
      if (threadId && onOpenDispatcher) open = () => guardLeave(() => onOpenDispatcher(threadId));
      break;
    }
    case 'automation': {
      Icon = Clock;
      label = provenance.name
        ? t('chat.provenance.automationNamed', { name: provenance.name })
        : t('chat.provenance.automation');
      const { automationId } = provenance;
      if (automationId) open = () => guardLeave(() => navigate(`/automations?id=${encodeURIComponent(automationId)}`));
      break;
    }
    case 'channel': {
      // The app's own logo, as its tab in the file panel shows it.
      Icon = MessageCircle;
      const domain = messagingAppDomain(provenance.platform);
      if (domain) logo = <Favicon domain={domain} size={14} />;
      label = t('chat.provenance.viaChannel', { platform: messagingAppName(provenance.platform) });
      break;
    }
    case 'main_agent':
      Icon = CornerDownRight;
      label = t('chat.provenance.fromMainAgent');
      break;
    case 'analyst_report':
    case 'task_report':
    case 'command_report':
      // The list draws a report as a line of its own (`ReportRow`), never as
      // a caption over a bubble.
      return null;
  }
  // A long name truncates; the whole caption is a hover away.
  title ??= typeof label === 'string' ? label : undefined;

  const content = (
    <>
      {logo ?? (Icon && <Icon aria-hidden="true" className="h-3 w-3 shrink-0" />)}
      <span className="min-w-0 truncate">{label}</span>
    </>
  );

  if (open && !readOnly) {
    return (
      <button type="button" className={cn(LINKED, className)} title={title} onClick={open}>
        {content}
        <ArrowUpRight aria-hidden="true" className="h-3 w-3 shrink-0" />
      </button>
    );
  }
  return (
    <span className={cn(CAPTION, className)} title={title}>
      {content}
    </span>
  );
}

/**
 * The mark on a message a turn already running took in, rather than one that
 * opened a turn: whoever sent it, the user here, an agent or a channel. In the
 * ink and size of the line it sits on.
 */
export function SteeredMark({ className }: { className?: string }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <span className={cn('inline-flex shrink-0 items-center gap-1.5', className)} title={t('chat.provenance.steeredHint')}>
      <Merge aria-hidden="true" className="h-3 w-3 shrink-0" />
      {t('chat.provenance.steered')}
    </span>
  );
}
