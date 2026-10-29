/**
 * A user-role message's bubble, under the caption naming who sent it when
 * someone other than the user here did, and marking it when a turn already
 * running took it in. The transcript and a subagent's opening prompt both
 * draw it, so a caption sits the same distance above its bubble in each.
 */
import React from 'react';
import { cn } from '@/lib/utils';
import type { MessageProvenance } from '@/types/chat';
import { CAPTION, MessageProvenanceLabel, SteeredMark } from './MessageProvenance';

interface UserBubbleProps {
  provenance?: MessageProvenance;
  /** A turn already running took it in. */
  steered?: boolean;
  readOnly?: boolean;
  isMobile?: boolean;
  /** Goes on the outermost element: the captioned column, or the bubble alone. */
  className?: string;
  children: React.ReactNode;
}

export function UserBubble({ provenance, steered, readOnly, isMobile, className, children }: UserBubbleProps): React.ReactElement {
  const captioned = !!provenance || !!steered;
  // Every parent right-aligns the bubble in a column flexbox, which sizes it to
  // its widest unbreakable run of text (a comma-joined list, a URL), and an
  // end-aligned item wider than its column spills out the left, where nothing
  // can scroll to it. Capping it at the column makes `break-word` split the run.
  const bubble = (
    <div
      className={cn('max-w-full overflow-hidden rounded-xl wrap-break-word', isMobile ? 'px-3 py-2' : 'px-4 py-3', !captioned && className)}
      style={{ backgroundColor: 'var(--color-bg-elevated)', color: 'var(--color-text-primary)' }}
    >
      {children}
    </div>
  );
  if (!captioned) return bubble;
  // The sender keeps the bubble's edge, where it sits on a message not steered.
  return (
    <div className={cn('flex min-w-0 max-w-full flex-col items-end gap-1.5', className)}>
      <div className="flex min-w-0 max-w-full items-center gap-3">
        {steered && <SteeredMark className={CAPTION} />}
        {provenance && <MessageProvenanceLabel provenance={provenance} readOnly={readOnly} />}
      </div>
      {bubble}
    </div>
  );
}
