import React from 'react';
import { Plus } from 'lucide-react';
import { SwapCell } from '@/components/ui/crossfade';
import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';
import type { PinState } from './commentModel';
import './PinBadge.css';

/**
 * What a pin's disc holds: its number, the Loader while the agent is on it,
 * or a plus for a comment not yet written. `working` left out draws the
 * number alone, for a surface where the agent is never at work on it.
 */
export function PinFace({ number, working, label, size }: {
  number: number | null;
  working?: boolean;
  /** Read out for the Loader, where the pin itself is not hidden. */
  label?: string;
  size: number;
}): React.ReactElement {
  if (number === null) return <Plus style={{ width: size, height: size }} strokeWidth={2.5} />;
  if (working === undefined) return <>{number}</>;
  return (
    <SwapCell swapKey={working ? 'working' : 'number'} scale>
      {working ? <Loader size={size} label={label} style={{ color: 'inherit' }} /> : number}
    </SwapCell>
  );
}

/**
 * A comment's pin as it reads beside text, in a list, a card's head or the
 * chat: its number in the state the pin on the page wears, so the two are
 * matched at a glance, and the unread dot when the pin wears it.
 */
export function PinBadge({ number, state, working, unread, className }: {
  number: number | null;
  state: PinState | 'draft';
  working?: boolean;
  /** An agent message the owner has not opened; the caller says so in words. */
  unread?: boolean;
  className?: string;
}): React.ReactElement {
  return (
    <span
      className={cn('page-comment-pin is-static', className)}
      data-pin-state={state}
      data-working={working || undefined}
      data-unread={unread || undefined}
      aria-hidden
    >
      <span className="page-comment-pin-disc">
        <PinFace number={number} working={working} size={10} />
      </span>
    </span>
  );
}
