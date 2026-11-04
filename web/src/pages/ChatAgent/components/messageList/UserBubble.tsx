/**
 * A user-role message's bubble, under the caption naming who sent it when
 * someone other than the user here did, and marking it when a turn already
 * running took it in. The transcript and a subagent's opening prompt both
 * draw it, so a caption sits the same distance above its bubble in each.
 */
import React, { useState } from 'react';
import { Disclosure } from '@/components/ui/Disclosure';
import { cn } from '@/lib/utils';
import type { MessageProvenance } from '@/types/chat';
import { CAPTION, MessageProvenanceLabel, SteeredMark } from './MessageProvenance';

interface UserBubbleProps {
  provenance?: MessageProvenance;
  /** A turn already running took it in. */
  steered?: boolean;
  readOnly?: boolean;
  isMobile?: boolean;
  /** Goes on the outermost element, the column the caption and bubble sit in. */
  className?: string;
  children: React.ReactNode;
}

const CAPTION_BOX: React.CSSProperties = { minWidth: 0, maxWidth: '100%' };

export function UserBubble({ provenance, steered, readOnly, isMobile, className, children }: UserBubbleProps): React.ReactElement {
  const captioned = !!provenance || !!steered;
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
  // caption, so the fold is mounted only once there is one.
  return (
    <div className={cn('flex min-w-0 max-w-full flex-col items-end', className)}>
      {captioned && (
        <Disclosure open appear={!captionedFirst} className="pb-1.5" style={CAPTION_BOX}>
          {/* The sender keeps the bubble's edge, where it sits on a message not steered. */}
          <div className="flex min-w-0 max-w-full items-center gap-3">
            {steered && <SteeredMark className={CAPTION} />}
            {provenance && <MessageProvenanceLabel provenance={provenance} readOnly={readOnly} />}
          </div>
        </Disclosure>
      )}
      <div
        className={cn('max-w-full overflow-hidden rounded-xl wrap-break-word', isMobile ? 'px-3 py-2' : 'px-4 py-3')}
        style={{ backgroundColor: 'var(--color-bg-elevated)', color: 'var(--color-text-primary)' }}
      >
        {children}
      </div>
    </div>
  );
}
