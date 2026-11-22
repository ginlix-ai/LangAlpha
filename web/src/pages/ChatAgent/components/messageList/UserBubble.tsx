/**
 * A user-role message's bubble, under the caption naming who sent it when
 * someone other than the user here did, and marking it when a turn already
 * running took it in. The transcript and a subagent's opening prompt both
 * draw it, so a caption sits the same distance above its bubble in each.
 *
 * In a shared workspace's thread the side says whose it is, as in a group
 * chat: the reader's own sits at the end in the usual bubble, its sender said
 * to a screen reader only, and another member's sits at the start in a
 * quieter outlined one under their name, beside their `SenderPicture`.
 */
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Disclosure } from '@/components/ui/Disclosure';
import { PersonAvatar } from '@/components/organization/PersonAvatar';
import { cn } from '@/lib/utils';
import { personLabel } from '@/lib/sharedThreads';
import type { MessageProvenance } from '@/types/chat';
import type { BubbleSender } from '../../session/sharedThread';
import { CAPTION, MessageProvenanceLabel, SteeredMark } from './MessageProvenance';
import './UserBubble.css';

interface UserBubbleProps {
  provenance?: MessageProvenance;
  /** Who sent it, in a shared workspace's thread; never beside `provenance`. */
  sender?: BubbleSender;
  /** Another member's message right after another of theirs: the name over
   *  the first of the run stands for this one too. */
  continues?: boolean;
  /** A turn already running took it in. */
  steered?: boolean;
  readOnly?: boolean;
  isMobile?: boolean;
  /** Goes on the outermost element, the column the caption and bubble sit in. */
  className?: string;
  children: React.ReactNode;
}

const CAPTION_BOX: React.CSSProperties = { minWidth: 0, maxWidth: '100%' };
const OWN_FILL: React.CSSProperties = { backgroundColor: 'var(--color-shell-fill, var(--color-bg-elevated))', color: 'var(--color-text-primary)' };
/** Another member's bubble takes its surface from UserBubble.css. */
const THEIRS_INK: React.CSSProperties = { color: 'var(--color-text-primary)' };
/** Another member's picture sits beside their bubble with their name level
 *  with its middle, so the picture's column and the name's line share it. */
const PICTURE_PX = 24;
const PICTURE_COLUMN: React.CSSProperties = { width: PICTURE_PX };
const NAME_LINE: React.CSSProperties = { height: PICTURE_PX };

/** The column beside another member's message holding their picture, drawn
 *  on the first message of a run of theirs; the rest of the run keeps its
 *  width, so their bubbles line up. */
export function SenderPicture({ sender, continues }: { sender: BubbleSender; continues?: boolean }): React.ReactElement {
  return (
    <div className="shrink-0" style={PICTURE_COLUMN} data-testid="message-sender-avatar">
      {!continues && <PersonAvatar name={sender.name} avatarUrl={sender.avatarUrl} size={PICTURE_PX} />}
    </div>
  );
}

export function UserBubble({ provenance, sender, continues, steered, readOnly, isMobile, className, children }: UserBubbleProps): React.ReactElement {
  const { t } = useTranslation();
  const theirs = !!sender && !sender.mine;
  // The name is drawn over another member's message that opens a run of
  // theirs; the reader's own and the rest of a run say it to a screen reader.
  const named = theirs && !continues ? sender : undefined;
  const captioned = !!provenance || !!steered || !!named;
  // Only a caption arriving after the bubble (a running turn taking the
  // message in) unfolds; one there from the start is drawn open.
  const [captionedFirst] = useState(captioned);
  // Every parent right-aligns the bubble in a column flexbox, which sizes it to
  // its widest unbreakable run of text (a comma-joined list, a URL), and an
  // end-aligned item wider than its column spills out the left, where nothing
  // can scroll to it. Capping it at the column makes `break-word` split the run.
  // The bubble sits in the column whether or not a caption is over it, and a
  // caption arriving folds open above it: the bubble moves down rather than
  // jumping, and what it holds stays mounted, so a pending comment list fades
  // to full rather than being drawn anew at full. Most bubbles never have a
  // caption, so the fold is mounted only once there is one. The open card
  // styles draw the bubble unfilled inside their edge (UserBubble.css).
  return (
    <div className={cn('flex min-w-0 max-w-full flex-col', theirs ? 'items-start' : 'items-end', className)}>
      {sender && !named && <span className="sr-only" data-testid="message-sender">{personLabel(t, sender)}</span>}
      {captioned && (
        <Disclosure open appear={!captionedFirst} className="pb-1.5" style={CAPTION_BOX}>
          {/* The sender keeps the bubble's edge, where it sits on a message not steered. */}
          <div className="flex min-w-0 max-w-full items-center gap-3">
            {named && (
              <span className={cn(CAPTION, 'font-medium text-(--color-text-secondary)')} style={NAME_LINE} data-testid="message-sender">
                <span className="min-w-0 truncate">{personLabel(t, named)}</span>
              </span>
            )}
            {steered && <SteeredMark className={CAPTION} />}
            {provenance && <MessageProvenanceLabel provenance={provenance} readOnly={readOnly} />}
          </div>
        </Disclosure>
      )}
      <div
        className={cn('pf-bubble max-w-full overflow-hidden rounded-[var(--cs-radius-box,0.75rem)] wrap-break-word', theirs && 'pf-bubble-theirs', isMobile ? 'px-3 py-2' : 'px-4 py-3')}
        style={theirs ? THEIRS_INK : OWN_FILL}
      >
        {children}
      </div>
    </div>
  );
}
